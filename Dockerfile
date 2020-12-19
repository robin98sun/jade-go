FROM debian
COPY ./app /app
COPY ../jade-ui/build /ui
EXPOSE 8080
ENTRYPOINT /app