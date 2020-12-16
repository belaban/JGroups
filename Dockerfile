
## Builds an image containing JGroups.

## **********************************************************
## Make sure you have JGroups compiled (ant) before doing so!
## **********************************************************

# Build: docker build -f Dockerfile -t belaban/jgrp .
# Push: docker push belaban/jgrp


FROM adoptopenjdk/openjdk11:jre as build-stage
RUN apt-get update ; apt-get install -y git ant net-tools netcat iputils-ping

# For the runtime, we only need a JRE (smaller footprint)
FROM adoptopenjdk/openjdk11:jre as make-dirs
LABEL maintainer="Bela Ban (belaban@mailbox.org)"
RUN useradd --uid 1000 --home /opt/jgroups --create-home --shell /bin/bash jgroups
RUN echo root:root | chpasswd ; echo jgroups:jgroups | chpasswd
RUN printf "\njgroups ALL=(ALL) NOPASSWD: ALL\n" >> /etc/sudoers
# EXPOSE 7800-7900:7800-7900 9000-9100:9000-9100
ENV HOME /opt/jgroups
ENV PATH $PATH:$HOME/JGroups/bin
ENV JGROUPS_HOME=$HOME/JGroups
WORKDIR /opt/jgroups

COPY --from=build-stage /bin/ping /bin/netstat /bin/nc /bin/
COPY --from=build-stage /sbin/ifconfig /sbin/
COPY README $JGROUPS_HOME/
COPY ./classes $JGROUPS_HOME/classes
COPY ./lib $JGROUPS_HOME/lib
COPY ./bin $JGROUPS_HOME/bin

RUN chown -R jgroups.jgroups $HOME/*

# Run everything below as the jgroups user. Unfortunately, USER is only observed by RUN, *not* by ADD or COPY !!
USER jgroups

RUN chmod u+x $HOME/*
CMD clear && cat $HOME/JGroups/README && /bin/bash


