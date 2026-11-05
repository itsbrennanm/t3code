import { ThreadId } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as ProcessRunner from "../processRunner.ts";

interface OwnedWorkerJobsShape {
  readonly cancel: (threadId: ThreadId, createdBefore: number) => Effect.Effect<void>;
}

export const OwnedWorkerJobs = Context.Reference<OwnedWorkerJobsShape>("t3/OwnedWorkerJobs", {
  defaultValue: () => ({ cancel: () => Effect.void }),
});

export const make = Effect.gen(function* () {
  const environment = yield* HostProcess.Environment;
  const runner = yield* ProcessRunner.ProcessRunner;
  const command = environment.T3_WORKER_JOB_COMMAND;
  if (command === undefined) return OwnedWorkerJobs.defaultValue();
  if (!command.startsWith("/")) {
    yield* Effect.logWarning("Owned worker cancellation is disabled: command must be absolute");
    return OwnedWorkerJobs.defaultValue();
  }
  return {
    cancel: Effect.fn("OwnedWorkerJobs.cancel")(function* (
      threadId: ThreadId,
      createdBefore: number,
    ) {
      const result = yield* runner
        .run({
          command,
          args: ["cancel-owner", "--owner", threadId, "--created-before", String(createdBefore)],
          timeout: "20 seconds",
          maxOutputBytes: 8_192,
        })
        .pipe(Effect.option);
      if (result._tag === "None" || result.value.code !== 0) {
        yield* Effect.logWarning("Owned worker cancellation was not confirmed", { threadId });
      }
    }),
  } satisfies OwnedWorkerJobsShape;
});

export const layer = Layer.effect(OwnedWorkerJobs, make).pipe(Layer.provide(ProcessRunner.layer));
