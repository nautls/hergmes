<script setup lang="ts">
import { Relay, type Key } from "@hergmes/sdk";
import { computed } from "vue";
import HistoryList from "./HistoryList.vue";
import { amount, erg, keyText, short, tokens } from "./format.ts";
import { useHandle } from "./use-handle.ts";

const props = defineProps<{ relay: Relay; target: Key; boxes: boolean }>();
defineEmits<{ remove: [] }>();

// Opens again when the relay or the key changes; closes with the component.
const { state } = useHandle(() =>
  "address" in props.target
    ? props.relay.subscribe(props.target, { boxes: props.boxes })
    : props.relay.subscribe(props.target),
);

const address = computed(() => ("address" in props.target ? props.target.address : undefined));
const live = computed(() => state.value?.phase === "live");
const spendableErgs = computed(
  () => state.value?.boxes?.spendable.reduce((sum, box) => sum + box.value, 0n) ?? 0n,
);
const confirmedIds = computed(
  () => new Set(state.value?.boxes?.confirmed.map((box) => box.boxId) ?? []),
);
</script>

<template>
  <section class="card" :class="{ stale: !live }">
    <div class="card-head">
      <code :title="keyText(target)">{{ short(keyText(target)) }}</code>
      <span class="tag"
        >{{ address ? "address" : "token" }}{{ boxes ? "" : ", balance only" }}</span
      >
      <span class="phase" :data-phase="state?.phase">{{ state?.phase }}</span>
      <button class="link" @click="$emit('remove')">Remove</button>
    </div>

    <p v-if="state?.error" class="error">
      {{ state.error.code }}: {{ state.error.message }}
      <template v-if="state.error.retryAfter !== undefined">
        (again in {{ state.error.retryAfter }} s)
      </template>
    </p>

    <dl v-if="state?.balance" class="balance">
      <dt>Confirmed</dt>
      <dd>{{ erg(state.balance.confirmed.nanoErgs) }}</dd>
      <dt>Pending</dt>
      <dd :class="{ muted: state.balance.pending.nanoErgs === 0n }">
        {{ erg(state.balance.pending.nanoErgs, true) }}
      </dd>
      <template v-for="[id, value] in tokens(state.balance.confirmed)" :key="id">
        <dt>
          <code :title="id">{{ short(id) }}</code>
        </dt>
        <dd>{{ amount(value) }}</dd>
      </template>
      <template v-for="[id, value] in tokens(state.balance.pending)" :key="`p${id}`">
        <dt>
          <code :title="id">{{ short(id) }}</code> pending
        </dt>
        <dd>{{ amount(value, true) }}</dd>
      </template>
    </dl>

    <template v-if="state?.boxes">
      <h3>
        Spendable: {{ state.boxes.spendable.length }} boxes,
        {{ erg(spendableErgs) }}
        <span class="muted">
          last change at {{ state.boxes.lastChange.height.toLocaleString() }}
        </span>
      </h3>
      <ul class="boxes">
        <li v-for="box in state.boxes.spendable" :key="box.boxId">
          <code :title="box.boxId">{{ short(box.boxId) }}</code>
          <span>{{ erg(box.value) }}</span>
          <span v-if="box.assets.length" class="muted">
            {{ box.assets.length }} token{{ box.assets.length === 1 ? "" : "s" }}
          </span>
          <span v-if="!confirmedIds.has(box.boxId)" class="tag pending">pending</span>
        </li>
      </ul>

      <template v-if="state.boxes.pending.length">
        <h3>Pending txs</h3>
        <ul class="boxes">
          <li v-for="tx in state.boxes.pending" :key="tx.txId">
            <code :title="tx.txId">{{ short(tx.txId) }}</code>
            <span>+{{ tx.created.length }} / −{{ tx.spent.length }} boxes</span>
          </li>
        </ul>
      </template>
    </template>

    <HistoryList v-if="address" :relay="relay" :address="address" />
  </section>
</template>
