FROM node:22-slim
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
# next.config.mjs bakes the API address into its rewrites at build time
ARG SCOUT_API_URL=http://api:8001
ENV SCOUT_API_URL=$SCOUT_API_URL NODE_ENV=production
RUN npm run build
EXPOSE 3000
CMD ["npm", "run", "start"]
