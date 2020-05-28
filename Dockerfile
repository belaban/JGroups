
# This is the smallest JDK 11 image I could find; alpine has smaller images, but not for JDK 11 (only for 8)
FROM adoptopenjdk/openjdk11

LABEL maintainer="Bela Ban (belaban@mailbox.org)"

RUN apt-get update ; apt-get install -y git ant

# Create a user and group used to launch processes
# The user ID 1000 is the default for the first "regular" user on Fedora/RHEL,
# so there is a high chance that this ID will be equal to the current user
# making it easier to use volumes (no permission issues)
RUN useradd --uid 1000 --home /opt/jgroups --create-home --shell /bin/bash jgroups
RUN echo root:root | chpasswd ; echo jgroups:jgroups | chpasswd

RUN printf "\njgroups ALL=(ALL) NOPASSWD: ALL\n" >> /etc/sudoers

EXPOSE 7800-7900:7800-7900 9000-9100:9000-9100

ENV HOME /opt/jgroups
ENV PATH $PATH:$HOME/JGroups/bin
ENV JGROUPS_HOME=$HOME/JGroups

WORKDIR /opt/jgroups

## Download and build JGroups src code
RUN git clone https://github.com/belaban/JGroups.git
RUN cd JGroups && ant retrieve ; ant compile




FROM adoptopenjdk/openjdk11:jre
RUN useradd --uid 1000 --home /opt/jgroups --create-home --shell /bin/bash jgroups
RUN echo root:root | chpasswd ; echo jgroups:jgroups | chpasswd
RUN printf "\njgroups ALL=(ALL) NOPASSWD: ALL\n" >> /etc/sudoers
EXPOSE 7800-7900:7800-7900 9000-9100:9000-9100
ENV HOME /opt/jgroups
ENV PATH $PATH:$HOME/JGroups/bin
ENV JGROUPS_HOME=$HOME/JGroups
WORKDIR /opt/jgroups

COPY --from=0 /opt/jgroups/JGroups /opt/jgroups/JGroups


RUN chown -R jgroups.jgroups $HOME/*

# Run everything below as the jgroups user. Unfortunately, USER is only observed by RUN, 
# *not* by ADD or COPY !!
USER jgroups


RUN chmod u+x $HOME/*


CMD clear && cat $HOME/JGroups/README && /bin/bash


