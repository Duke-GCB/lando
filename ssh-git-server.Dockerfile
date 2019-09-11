# Slightly modified from https://github.com/openshift-qe/ssh-git-docker/tree/master/ssh-git-openshift/Dockerfile
FROM fedora:latest

RUN dnf -y install openssh-server git
RUN dnf -y install ed # needed to edit passwd and group
RUN dnf clean all

# setup openssh
RUN sed -i "s/#PasswordAuthentication yes/PasswordAuthentication no/" /etc/ssh/sshd_config
# SSHd 7.4+ (maybe earlier) this is not needed, see
#  https://lists.mindrot.org/pipermail/openssh-unix-dev/2017-August/036168.html
# RUN sed -i 's/#UsePrivilegeSeparation.*$/UsePrivilegeSeparation no/' /etc/ssh/sshd_config

RUN sed -i 's/#Port.*$/Port 2022/' /etc/ssh/sshd_config
RUN chmod 775 /var/run
RUN rm -f /var/run/nologin

# setup git user
RUN adduser --system -s /bin/bash -u 1234321 -g 0 git # uid to replace later
RUN chmod 775 /etc/ssh /home # keep writable for openshift user group (root)
RUN chmod 660 /etc/ssh/sshd_config
RUN chmod 664 /etc/passwd /etc/group # to help uid fix
RUN ln -s /home/git /repos # nicer repo url

EXPOSE 2022

USER git
CMD echo -e ",s/1234321/`id -u`/g\\012 w" | ed -s /etc/passwd && \
    mkdir -p /home/git/.ssh && \
    touch /home/git/.ssh/authorized_keys && \
    echo ${PUBLIC_KEY} > /home/git/.ssh/authorized_keys && \
    chmod 700 /home/git/.ssh && \
    chmod 600 /home/git/.ssh/authorized_keys && \
    ssh-keygen -A && \
    exec /usr/sbin/sshd -D
