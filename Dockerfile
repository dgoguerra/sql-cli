FROM node:12.16.1-alpine as build

# Install git (for github.com NPM deps), build tools (for node-gyp builds)
# and libsecret-dev (for keytar package).
RUN apk add --no-cache git openssh python alpine-sdk libsecret-dev

WORKDIR /app
COPY package.json package-lock.json ./
RUN set -x && npm ci --prod && npm cache clean --force

FROM node:12.16.1-alpine as app
# keytar package still needs libsecret at runtime
RUN apk add --no-cache libsecret
WORKDIR /app
COPY --from=build /app .
COPY . .
ENTRYPOINT ["src/index.js"]
