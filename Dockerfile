FROM camunda/camunda-bpm-platform:7.16.0
USER root
COPY --chown=camunda:camunda ./engine-rest/web.xml /camunda/webapps/engine-rest/WEB-INF/web.xml
