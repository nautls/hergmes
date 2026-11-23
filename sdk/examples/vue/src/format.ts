import type { Assets, Key } from "@hergmes/sdk";

/** nanoERG as ERG, trailing zeros dropped; with `signed`, a `+` on gains. */
export function erg(nanoErgs: bigint, signed = false): string {
  const sign = nanoErgs < 0n ? "-" : signed && nanoErgs > 0n ? "+" : "";
  const abs = nanoErgs < 0n ? -nanoErgs : nanoErgs;
  const fraction = (abs % 1_000_000_000n).toString().padStart(9, "0").replace(/0+$/, "");
  return `${sign}${(abs / 1_000_000_000n).toString()}${fraction ? `.${fraction}` : ""} ERG`;
}

/** A token amount in its base units: a token's decimals are not the relay's to know. */
export function amount(value: bigint, signed = false): string {
  return `${signed && value > 0n ? "+" : ""}${value.toString()}`;
}

/** An id or address cut to its ends. */
export function short(id: string): string {
  return id.length > 16 ? `${id.slice(0, 8)}…${id.slice(-6)}` : id;
}

/** The tokens of `assets` with a non-zero amount. */
export function tokens(assets: Assets): [string, bigint][] {
  return [...assets.tokens].filter(([, value]) => value !== 0n);
}

/** A token id is 64 hex digits; anything else is taken for an address. */
export function parseKey(text: string): Key {
  const value = text.trim();
  return /^[0-9a-f]{64}$/i.test(value) ? { tokenId: value.toLowerCase() } : { address: value };
}

export function keyText(key: Key): string {
  if ("address" in key) return key.address;
  return "tokenId" in key ? key.tokenId : key.template;
}
