<script setup lang="ts">
import type { Relay } from "@hergmes/sdk";
import { amount, erg, short, tokens } from "./format.ts";
import { useHandle } from "./use-handle.ts";

const props = defineProps<{ relay: Relay; address: string }>();

// The first page is read again whenever the address's balance changes.
const { state, handle } = useHandle(() =>
  props.relay.history(props.address, { refresh: true, limit: 10 }),
);

function more() {
  void handle.value?.more();
}
</script>

<template>
  <details>
    <summary>History</summary>
    <p v-if="state?.error" class="error">{{ state.error.code }}: {{ state.error.message }}</p>
    <ul class="boxes">
      <li v-for="item in state?.items" :key="item.txId">
        <code :title="item.txId">{{ short(item.txId) }}</code>
        <span>{{ erg(item.delta.nanoErgs, true) }}</span>
        <span v-for="[id, value] in tokens(item.delta)" :key="id" class="muted">
          {{ amount(value, true) }} {{ short(id) }}
        </span>
        <span v-if="item.height === undefined" class="tag pending">pending</span>
        <span v-else class="muted">
          {{ item.height.toLocaleString() }}
          <template v-if="item.timestamp"
            >· {{ new Date(item.timestamp).toLocaleString() }}</template
          >
        </span>
      </li>
    </ul>
    <button v-if="state && !state.end" class="link" :disabled="state.loading" @click="more">
      {{ state.loading ? "Loading…" : "Older" }}
    </button>
  </details>
</template>
