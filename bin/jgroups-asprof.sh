## Uses async-profiler to measure CPU stats (can be changed) into out.html
## async-profiler needs to be in a dir called 'async-profiler' in the home dir

#!/bin/bash

AP="-agentpath:$HOME/async-profiler/lib/libasyncProfiler.dylib=start,event=cpu,file=out.html"

jgroups.sh $AP $*

