FROM debian
COPY ./app /app
COPY ./ui /ui
EXPOSE 8080
ENTRYPOINT /app