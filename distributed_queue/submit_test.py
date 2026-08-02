import zmq

context = zmq.Context()

socket = context.socket(zmq.PUSH)
socket.connect("tcp://localhost:5558")

message = {
    "type": "process_data",
    "payload": {
        "input": "hello"
    }
}

socket.send_json(message)

print("Task submitted!")

socket.close()
context.term()