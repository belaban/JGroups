#!/usr/bin/env bash
# Mirrors Ant's "all-tests-parallel" target (build.xml:536-555): five lanes run
# concurrently, suites inside a lane sequentially (port ranges are lane-specific).
# Maven cannot express this in a single-module POM (-T only parallelises modules).
#
# Usage: ./run-tests-parallel.sh [maven flags...]
#   e.g. ./run-tests-parallel.sh -o -Dtest.failure.ignore=false
# HTML report: tmp/test-results/html/index.html (generate with
#              mvn antrun:run@test-reports after the run)
set -u
cd "$(dirname "$0")" || exit 1

# Extra flags are forwarded to every Maven invocation.
MVN_ARGS=("$@")

mvn ${MVN_ARGS[@]+"${MVN_ARGS[@]}"} test-compile || exit 1

# Ant's all-tests-parallel depends on "delete-reports". "mvn test-compile" does not
# clean (so single-suite runs can be combined), so do it here before the lanes start.
rm -rf tmp/test-results

run_lane() {
    for suite in "$@"; do
        mvn ${MVN_ARGS[@]+"${MVN_ARGS[@]}"} surefire:test@"$suite" || return 1
    done
}

run_lane functional                                  &
p1=$!
run_lane udp udp-new                                 &
p2=$!
run_lane tcp tcp-new tcp-nio tcp-nio-new             &
p3=$!
run_lane encrypt time-sensitive byteman              &
p4=$!
run_lane stack-independent                           &
p5=$!

rc=0
for pid in "$p1" "$p2" "$p3" "$p4" "$p5"; do
    wait "$pid" || rc=1
done
exit "$rc"
