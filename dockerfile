# Install python package
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Run the script when the container launches
CMD ["/bin/sh", "-c", "python src/api/main_yearly.py && python command2.py && python src/pipeline/beam_pipeline.py"]