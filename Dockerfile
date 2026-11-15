# One image for both the boss and the workers. The Compose file gives each
# service its own command.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mapreduce/ mapreduce/
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. mapreduce/mapreduce.proto

ENV PYTHONUNBUFFERED=1

# The containers run as root, but the shared directory belongs to the user
# on the host. A umask of 0 keeps everything the containers create there
# readable, writable and removable by that user.
ENTRYPOINT ["sh", "-c", "umask 0000 && exec \"$@\"", "--"]
CMD ["python", "-m", "mapreduce.boss"]
