FROM apify/actor-node:18

# Copy package.json and package-lock.json into the container
COPY package.json ./

# Install dependencies
RUN npm --quiet set progress=false \
    && npm install --only=prod --no-optional \
    && echo "Installed NPM packages:" \
    && (npm list || true) \
    && echo "Node.js version:" \
    && node --version \
    && echo "NPM version:" \
    && npm --version

# Copy the rest of the application into the container
COPY . ./

# Run the application
CMD ["node", "src/apify-main.js"]