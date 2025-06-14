import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.controllers import candidate_controller, sample_controller

app = FastAPI(title="Candidate Management API")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Include routers
app.include_router(candidate_controller.router, prefix="/api/v1")
app.include_router(sample_controller.router, prefix="/api/v1", tags=["sample"])

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)