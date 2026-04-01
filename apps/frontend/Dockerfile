FROM node:18-alpine
WORKDIR /app
COPY . .
RUN npm install
RUN npm run build || true
EXPOSE 3000
CMD ["npm", "start"]
