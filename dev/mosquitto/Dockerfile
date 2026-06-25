FROM eclipse-mosquitto:2.0.18-openssl
COPY mosquitto.conf mosquitto_with_user.conf mosquitto_pwd /configs/
COPY entrypoint.sh /entrypoint.sh
USER root
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
