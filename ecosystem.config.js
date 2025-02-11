module.exports = {
  apps: [
    {
      name: 'app-logs', // Change this to the name of your application
      script: './app-logs', // The name of the Go binary you built
      instances: 1, // Number of instances to run
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      env: {
        NODE_ENV: 'production',
      },
    },
  ],
};
