"""SSRF guard for all server-issued HTTP (connectivity tester AND api_source).

test_connection and api_source let users make this process issue requests to
arbitrary hosts, which is a Server-Side Request Forgery primitive. By default
we deny private, loopback, link-local, CGNAT and otherwise non-global ranges
(which covers cloud metadata endpoints such as 169.254.169.254). Deployments
that legitimately need to reach internal hosts opt in per host or CIDR via
``allow_hosts``.

Known v1 limitation (documented in the README): the check resolves DNS and
then hands the hostname to the HTTP client, which resolves again -- a
malicious DNS server could rebind between the two lookups. Pinning the
checked IP into the connection is planned for the server phase.
"""
from __future__ import annotations

import asyncio
import ipaddress
import socket
import urllib.parse

from pydantic import BaseModel, Field

from .errors import SSRFBlockedError

# Ranges not covered by the ipaddress convenience flags.
_EXTRA_BLOCKED_NETWORKS = (
    ipaddress.ip_network("100.64.0.0/10"),  # carrier-grade NAT
    ipaddress.ip_network("192.0.0.0/24"),  # IETF protocol assignments
)


class SSRFPolicy(BaseModel):
    """Per-deployment policy for server-issued requests.

    ``allow_hosts`` entries may be hostnames (case-insensitive exact match),
    single IPs, or CIDR blocks; matching targets are allowed even when they
    fall in a blocked range.
    """

    enabled: bool = True
    allow_hosts: list[str] = Field(default_factory=list)

    def _allowed_names(self) -> set[str]:
        names = set()
        for entry in self.allow_hosts:
            if "/" not in entry:
                names.add(entry.strip().strip("[]").lower())
        return names

    def _allowed_networks(self) -> list[ipaddress.IPv4Network | ipaddress.IPv6Network]:
        networks = []
        for entry in self.allow_hosts:
            entry = entry.strip()
            try:
                if "/" in entry:
                    networks.append(ipaddress.ip_network(entry, strict=False))
                else:
                    address = ipaddress.ip_address(entry.strip("[]"))
                    networks.append(ipaddress.ip_network(address))
            except ValueError:
                continue  # plain hostname entry, handled by _allowed_names
        return networks


def _is_blocked_address(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        address = address.ipv4_mapped
    if (
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
    ):
        return True
    return any(address in network for network in _EXTRA_BLOCKED_NETWORKS)


def find_blocked(host: str, ips: list[str], policy: SSRFPolicy) -> str | None:
    """Return a human-readable denial reason, or None when allowed.

    ``ips`` are the addresses ``host`` resolved to (or the literal itself).
    Pure/sync so the diagnostics ladder can reuse its own DNS rung's result.
    """
    if not policy.enabled:
        return None
    hostname = host.strip().strip("[]").lower()
    if hostname in policy._allowed_names():
        return None
    allowed_networks = policy._allowed_networks()
    for ip_text in ips:
        try:
            address = ipaddress.ip_address(ip_text)
        except ValueError:
            return f"could not parse resolved address {ip_text!r} for host {host!r}"
        if any(address in network for network in allowed_networks):
            continue
        if _is_blocked_address(address):
            return (
                f"host {host!r} resolves to {ip_text}, which is in a blocked "
                "range (private/loopback/link-local/metadata). Add the host to "
                "the SSRF allowlist if this is intentional."
            )
    return None


async def resolve_host(host: str, port: int | None = None) -> list[str]:
    """Resolve a hostname to its addresses. IP literals pass through."""
    bare = host.strip().strip("[]")
    try:
        ipaddress.ip_address(bare)
        return [bare]
    except ValueError:
        pass
    loop = asyncio.get_running_loop()
    infos = await loop.getaddrinfo(bare, port, type=socket.SOCK_STREAM)
    return sorted({info[4][0] for info in infos})


async def ensure_url_allowed(url: str, policy: SSRFPolicy) -> None:
    """Raise :class:`SSRFBlockedError` if the URL's host is denied by policy.

    DNS failures propagate as ``socket.gaierror`` so callers can categorize
    them as DNS errors rather than policy denials.
    """
    if not policy.enabled:
        return
    parts = urllib.parse.urlsplit(url)
    host = parts.hostname
    if not host:
        raise SSRFBlockedError(f"URL {url!r} has no host")
    if host.lower() in policy._allowed_names():
        return
    ips = await resolve_host(host, parts.port)
    reason = find_blocked(host, ips, policy)
    if reason is not None:
        raise SSRFBlockedError(reason)


def guarded_event_hooks(policy: SSRFPolicy) -> dict[str, list]:
    """httpx ``event_hooks`` that re-check the SSRF policy on every request.

    httpx fires request hooks once per hop, so this validates redirect
    targets too -- closing the bypass where a 302 to a private/metadata host
    would otherwise escape a guard that only checked the initial URL. Keeps
    the client's normal (proxy-aware) transport, unlike a custom transport.
    """

    async def _check(request) -> None:  # request: httpx.Request
        await ensure_url_allowed(str(request.url), policy)

    return {"request": [_check]}
