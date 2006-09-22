#This is a gnuplot file used for plotting performance test result graphs

set term png
set out "picname.png"
set data style boxes
set style fill solid border -1
set logscale xy
set yrange [1:100000]
set xrange [1:1000000]
set style fill solid 1.0
set boxwidth 0.10
set title "Throughput in msg/sec(cluster of 8 machines,udp.xml,JGroups 2.3)"
set xlabel "Message size"
set ylabel "Number of messages per sec"
set xtics ("100B" 100,"1K" 1000, "10K" 10000, "100K" 100000)
plot "perfdata.dat" using ($1):($2) lt rgb "#6495ED" t "1 sender",\
"perfdata.dat" using ($1*1.27):($3) lt rgb "#BA55D3" t "4 senders",\
"perfdata.dat" using ($1*1.63):($4) lt rgb "#FFF8DC" t "8 senders"


#where perfdata.dat looks something like this

#100		29754	30873	27161
#1000		15886	11354	15918
#10000	2013	314	624
#100000 	183	77	54
