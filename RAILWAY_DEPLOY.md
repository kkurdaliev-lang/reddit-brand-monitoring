# 🚀 Railway Deployment Guide

This guide will help you deploy the All-in-One Reddit Brand Monitor to Railway.

## 📋 Prerequisites

1. A [Railway](https://railway.app) account
2. Reddit API credentials
3. (Optional) HuggingFace API token for sentiment analysis

## 🔧 Deployment Steps

### 1. Push to GitHub

Make sure your code is in a GitHub repository with these files:
- `all_in_one_reddit_monitor.py` (main application)
- `requirements.txt` (dependencies)
- `Procfile` (tells Railway how to run the app)
- `runtime.txt` (specifies Python version)
- `railway.toml` (Railway configuration)

### 2. Connect to Railway

1. Go to [Railway](https://railway.app)
2. Click "Start a New Project"
3. Select "Deploy from GitHub repo"
4. Choose your repository

### 3. Set Environment Variables

In the Railway dashboard, go to your project → Variables tab and add:

**Required:**
```
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_client_secret
```

**Optional (for sentiment analysis):**
```
HF_API_TOKEN=your_huggingface_token
```

**The PORT variable is automatically set by Railway - don't add it manually.**

### 4. Get Reddit API Credentials

1. Go to https://www.reddit.com/prefs/apps
2. Click "Create App" or "Create Another App"
3. Choose "script" type
4. Enter any name and description
5. Copy the Client ID (under the app name) and Client Secret

### 5. Deploy

1. Railway will automatically detect this as a Python project
2. It will install dependencies from `requirements.txt`
3. It will run the app using the `Procfile`
4. The deployment typically takes 2-3 minutes

### 6. Access Your App

1. Once deployed, Railway will provide a public URL
2. Visit `https://your-app-name.railway.app`
3. You should see the Reddit Brand Monitor dashboard

## 🔍 Troubleshooting

### Build Errors

If Railway shows build errors:

1. **"Could not determine how to build"**: Make sure you have `requirements.txt` and `Procfile` in your repository root
2. **Python version issues**: Check that `runtime.txt` specifies a supported Python version (3.8-3.11)
3. **Dependency errors**: Verify all packages in `requirements.txt` are correctly spelled

### Runtime Errors

1. **App won't start**: Check the logs in Railway dashboard for error messages
2. **No Reddit data**: Verify your Reddit API credentials are correctly set in environment variables
3. **Database errors**: The app creates its own SQLite database - no additional setup needed

### Health Check

Railway will check `/health` endpoint to ensure the app is running properly. You can also visit this endpoint manually:
`https://your-app-name.railway.app/health`

## 🎯 What You Get

After successful deployment:

- **Live Reddit monitoring** running 24/7
- **Web dashboard** accessible from anywhere
- **Automatic data collection** from multiple sources
- **SQLite database** that persists between deployments
- **Export functionality** for CSV downloads

## 💡 Tips

1. **Monitor logs**: Use Railway's log viewer to monitor your app's performance
2. **Environment variables**: Never commit API keys to your repository - always use Railway's environment variables
3. **Database**: The SQLite database will persist between deployments in Railway's volume storage
4. **Custom domain**: Railway allows you to add a custom domain in the project settings

## 🔧 Customization

To modify the brands or subreddits being monitored, edit the `CONFIG` section in `all_in_one_reddit_monitor.py`:

```python
'brands': {
    'your_brand': r'[@#]?your_brand(?:\.com)?',
    'competitor': r'[@#]?competitor(?:\.com)?',
},
'subreddits': [
    "your_target_subreddit",
    "another_community",
    # add more subreddits
]
```

Then push the changes to GitHub and Railway will automatically redeploy.

## 🆘 Support

If you encounter issues:

1. Check Railway's deployment logs
2. Verify environment variables are set correctly
3. Test Reddit API credentials using the `/health` endpoint
4. Ensure your repository has all required files

Your Reddit Brand Monitor should now be live and collecting mentions 24/7! 🎉