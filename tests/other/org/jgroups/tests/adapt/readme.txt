This tests was originally designed for Spread, than it was 
adapted to JGroups. This version has been compiled with
Java 1.4.1 and jgroups 3.0.6.

It must have jgroups-all.jar included in the classpath.

Edit config.txt to your liking and start the Java client 
from the command line:
  > java org.jgroups.tests.adapt.Test receiver.txt

or if you wish you may create your own config file and pass
it as the argument to the application.

There are 2 predefined config files in JG_ROOT/conf: sender.txt and receiver.txt. They
can be modified and used with org.jgroups.tests.adapt.Test


Milan Prica (prica@deei.units.it)






