# Stage 1: Build Go executables
FROM golang:1.20 AS builder

# Set the working directory for building
WORKDIR /root

# Generate an SSH key pair
RUN mkdir -p /root/.ssh && ssh-keygen -t rsa -b 4096 -N "" -f /root/.ssh/id_rsa

# Copy the entire project into the container
COPY . .

WORKDIR /root/test

# Build executable for the tests
RUN go build

# Stage 2: Create the final image
FROM golang:1.20

# Install SSH server
RUN apt-get update && \
    apt-get install -y openssh-server && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir /var/run/sshd && \
    mkdir -p /root/.ssh && \
    touch /root/.ssh/authorized_keys

# Set public key from host machine (for ssh access)
ARG SSH_PUBLIC_KEY
RUN echo ${SSH_PUBLIC_KEY} > /root/.ssh/authorized_keys
RUN echo $(cat /root/.ssh/id_rsa.pub) >> /root/.ssh/authorized_keys

# Copy the built executables from the builder stage
COPY --from=builder /root/test /root/test
COPY --from=builder /root/test /root/test

# Copy the SSH key from the builder stage
COPY --from=builder /root/.ssh /root/.ssh

# Expose SSH port
EXPOSE 22

# Start SSH server
CMD ["/usr/sbin/sshd", "-D"]
