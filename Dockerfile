FROM camunda/camunda-bpm-platform:7.16.0
USER root
#RUN apk update
#RUN apk add libressl-dev
COPY --chown=camunda:camunda ./engine-rest/web.xml /camunda/webapps/engine-rest/WEB-INF/web.xml
#USER camunda
