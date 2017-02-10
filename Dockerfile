
FROM centos:7

MAINTAINER Humble Chirammal <hchiramm@redhat.com>

ADD glusterblock-provisioner /usr/local/bin/glusterblock-provisioner

ENTRYPOINT ["/usr/local/bin/glusterblock-provisioner"]