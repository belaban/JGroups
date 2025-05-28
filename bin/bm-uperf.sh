## Uses byteman to measure RTT times for UPerf

#!/bin/bash

bm.sh conf/scripts/RttTest/rtt.btm org.jgroups.tests.perf.UPerf $*