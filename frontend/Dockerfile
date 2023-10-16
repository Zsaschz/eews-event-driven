# stage1 as builder
FROM node:14 as builder

# copy the package.json to install dependencies
COPY /frontend/package.json /frontend/package-lock.json ./

# Install the dependencies and make the folder
RUN npm install && mkdir /frontend && mv ./node_modules ./frontend

WORKDIR /frontend

COPY /frontend .

# Backend IP
ARG REACT_APP_API
ENV REACT_APP_API 34.101.239.0:80

# Build the project and copy the files
RUN npm run build


FROM nginx:alpine

#!/bin/sh

COPY /nginx/webserver.conf /etc/nginx/nginx.conf
COPY /nginx/cert/ /etc/nginx/cert

## Remove default nginx index page
RUN rm -rf /usr/share/nginx/html/*

# Copy from the stahg 1
COPY --from=builder /frontend/build /usr/share/nginx/html

EXPOSE 80 443

ENTRYPOINT ["nginx", "-g", "daemon off;"]