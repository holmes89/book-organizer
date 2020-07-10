FROM golang:1.14-alpine AS base

FROM base as deps
WORKDIR "/book-organizer"
ADD *.mod *.sum ./
RUN go mod download

FROM deps AS build-env
ADD cmd ./cmd
ADD internal ./internal
ADD migrations ./migrations
ENV PORT 8080
EXPOSE 8080
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-w -X main.docker=true" -o server cmd/*.go
CMD ["./server"]

FROM alpine AS prod

WORKDIR /
ENV PORT 8080
EXPOSE 8080
COPY --from=build-env /book-organizer/server /
COPY --from=build-env /book-organizer/migrations /migrations
CMD ["/server"]
