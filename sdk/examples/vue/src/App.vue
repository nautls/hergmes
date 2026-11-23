<script setup lang="ts">
import { Relay, type BlockRef, type Key } from "@hergmes/sdk";
import { ref, shallowRef, watch, watchEffect } from "vue";
import KeyCard from "./KeyCard.vue";
import { parseKey } from "./format.ts";

interface Tracked {
  id: number;
  key: Key;
  boxes: boolean;
}

const saved = load();
const url = ref(new URLSearchParams(location.search).get("relay") ?? saved.url);
const urlDraft = ref(url.value);
const urlError = ref<string>();
const tracked = ref<Tracked[]>(saved.tracked);
const keyDraft = ref("");
const templateDraft = ref("");
const balanceOnly = ref(false);

// One relay for the page: one socket, shared by every key. A new URL closes the old relay.
const relay = shallowRef<Relay>();
const tip = shallowRef<BlockRef>();
watchEffect((onCleanup) => {
  urlError.value = undefined;
  let r: Relay;
  try {
    r = new Relay(url.value);
  } catch (e) {
    relay.value = undefined;
    urlError.value = e instanceof Error ? e.message : String(e);
    return;
  }
  relay.value = r;
  tip.value = r.tip;
  const off = r.onTip((t) => (tip.value = t));
  onCleanup(() => {
    off();
    r.close();
    tip.value = undefined;
  });
});

let nextId = Math.max(0, ...tracked.value.map((t) => t.id)) + 1;

function track() {
  if (!keyDraft.value.trim()) return;
  const key = parseKey(keyDraft.value);
  tracked.value.unshift({ id: nextId++, key, boxes: !("address" in key) || !balanceOnly.value });
  keyDraft.value = "";
}

// A template has its own input: one of 32 bytes is 64 hex digits, as a token id is.
function trackTemplate() {
  const template = templateDraft.value.trim().toLowerCase();
  if (!template) return;
  tracked.value.unshift({ id: nextId++, key: { template }, boxes: true });
  templateDraft.value = "";
}

function untrack(id: number) {
  tracked.value = tracked.value.filter((t) => t.id !== id);
}

// The relay URL and the tracked keys survive a reload.
const STORAGE = "hergmes-example";

function load(): { url: string; tracked: Tracked[] } {
  try {
    const raw = localStorage.getItem(STORAGE);
    if (raw) return JSON.parse(raw) as { url: string; tracked: Tracked[] };
  } catch {
    // No storage, or not ours: start empty.
  }
  return { url: "http://127.0.0.1:8080", tracked: [] };
}

watch(
  [url, tracked],
  () => {
    try {
      localStorage.setItem(STORAGE, JSON.stringify({ url: url.value, tracked: tracked.value }));
    } catch {
      // Storage is a convenience.
    }
  },
  { deep: true },
);
</script>

<template>
  <header>
    <h1>hergmes</h1>
    <span class="tip" :title="tip?.headerId">
      {{ tip ? `tip ${tip.height.toLocaleString()}` : "no tip yet" }}
    </span>
  </header>

  <form class="row" @submit.prevent="url = urlDraft.trim()">
    <input v-model="urlDraft" placeholder="Relay URL" aria-label="Relay URL" spellcheck="false" />
    <button :disabled="urlDraft.trim() === url">Connect</button>
  </form>
  <p v-if="urlError" class="error">{{ urlError }}</p>

  <form class="row" @submit.prevent="track">
    <input
      v-model="keyDraft"
      placeholder="Address or token id"
      aria-label="Address or token id"
      spellcheck="false"
    />
    <label><input v-model="balanceOnly" type="checkbox" /> Balance only</label>
    <button :disabled="!relay || !keyDraft.trim()">Track</button>
  </form>

  <form class="row" @submit.prevent="trackTemplate">
    <input
      v-model="templateDraft"
      placeholder="Contract template, in hex"
      aria-label="Contract template, in hex"
      spellcheck="false"
    />
    <button :disabled="!relay || !templateDraft.trim()">Track</button>
  </form>

  <main v-if="relay">
    <KeyCard
      v-for="t in tracked"
      :key="`${t.id}`"
      :relay="relay"
      :target="t.key"
      :boxes="t.boxes"
      @remove="untrack(t.id)"
    />
    <p v-if="tracked.length === 0" class="muted">
      Track an address, a token id or a contract template to follow its boxes, and an address's
      balance, as blocks and mempool txs arrive.
    </p>
  </main>
</template>
