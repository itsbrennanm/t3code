import { ThreadId } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { ChildProcessSpawner } from "effect/process";

import * as ProcessRunner from "../processRunner.ts";
import { make } from "./ownedWorkerJobs.ts";

it.effect("cancels only the exact thread through the configured job command", () => {
  const calls: ProcessRunner.ProcessRunInput[] = [];
  return Effect.gen(function* () {
    const jobs = yield* make;
    yield* jobs.cancel(ThreadId.make("mcp:66ab0549-d40e-4b65-ab06-96efd015bba8"), 1234);
    expect(calls).toHaveLength(1);
    expect(calls[0]?.command).toBe("/fixture/t3-job");
    expect(calls[0]?.args).toEqual([
      "cancel-owner",
      "--owner",
      "mcp:66ab0549-d40e-4b65-ab06-96efd015bba8",
      "--created-before",
      "1234",
    ]);
  }).pipe(
    Effect.provideService(HostProcess.Environment, { T3_WORKER_JOB_COMMAND: "/fixture/t3-job" }),
    Effect.provideService(ProcessRunner.ProcessRunner, {
      run: (input) =>
        Effect.sync(() => {
          calls.push(input);
          return {
            stdout: "{}",
            stderr: "",
            code: ChildProcessSpawner.ExitCode(0),
            timedOut: false,
            stdoutTruncated: false,
            stderrTruncated: false,
            stdoutInvalidUtf8: false,
            stderrInvalidUtf8: false,
          };
        }),
    }),
  );
});

it.effect("leaves worker jobs alone unless integration is configured", () =>
  Effect.gen(function* () {
    const jobs = yield* make;
    yield* jobs.cancel(ThreadId.make("owner"), 1234);
  }).pipe(
    Effect.provideService(HostProcess.Environment, {}),
    Effect.provideService(ProcessRunner.ProcessRunner, {
      run: () => Effect.die("unexpected command"),
    }),
  ),
);

it.effect("an unavailable helper does not prevent stopping the provider", () =>
  Effect.gen(function* () {
    const jobs = yield* make;
    yield* jobs.cancel(ThreadId.make("owner"), 1234);
  }).pipe(
    Effect.provideService(HostProcess.Environment, { T3_WORKER_JOB_COMMAND: "/fixture/missing" }),
    Effect.provideService(ProcessRunner.ProcessRunner, {
      run: () =>
        Effect.fail(
          new ProcessRunner.ProcessSpawnError({
            command: "/fixture/missing",
            argumentCount: 3,
            cause: "unavailable",
          }),
        ),
    }),
  ),
);
