
## The first stage is used to git-clone and build JGroups; this requires a JDK/javac/git/ant
FROM adoptopenjdk/openjdk11 as build-stage
RUN apt-get update ; apt-get install -y git ant net-tools netcat iputils-ping

## Download and build JGroups src code
RUN git clone https://github.com/belaban/JGroups.git
RUN cd JGroups && ant retrieve ; ant compile

# For the runtime, we only need a JRE (smaller footprint)
FROM adoptopenjdk/openjdk11:jre
LABEL maintainer="Bela Ban (belaban@mailbox.org)"
RUN useradd --uid 1000 --home /opt/jgroups --create-home --shell /bin/bash jgroups
RUN echo root:root | chpasswd ; echo jgroups:jgroups | chpasswd
RUN printf "\njgroups ALL=(ALL) NOPASSWD: ALL\n" >> /etc/sudoers
# EXPOSE 7800-7900:7800-7900 9000-9100:9000-9100
ENV HOME /opt/jgroups
ENV PATH $PATH:$HOME/JGroups/bin
ENV JGROUPS_HOME=$HOME/JGroups
WORKDIR /opt/jgroups

COPY --from=build-stage /JGroups /opt/jgroups/JGroups
COPY --from=build-stage /bin/ping /bin/netstat /bin/nc /bin/
COPY --from=build-stage /sbin/ifconfig /sbin/

RUN chown -R jgroups.jgroups $HOME/*

# Run everything below as the jgroups user. Unfortunately, USER is only observed by RUN, *not* by ADD or COPY !!
USER jgroups

RUN chmod u+x $HOME/*
CMD clear && cat $HOME/JGroups/README && /bin/bash


