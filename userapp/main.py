import uvicorn
from userapp import app

uvicorn.run(app, host="0.0.0.0", port=5000, log_level="info")
