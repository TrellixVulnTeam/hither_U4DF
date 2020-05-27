FROM node:12
EXPOSE 8080

RUN mkdir /src
COPY package.json /src/package.json
COPY server /src/server
WORKDIR /src
RUN npm install

# Remove the node user -- so it doesn't conflict with the host user
# In the future we will want to let injectUser.Dockerfile take care of this situation.
RUN userdel node

ENTRYPOINT npm run start-docker