from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/api/employee', methods=['POST'])
def add_employee():
    data = request.get_json()
    print(f"Received data: {data}")
    return jsonify({"status": "success", "data": data}), 201


if __name__ == '__main__':
    app.run(port=8181, debug=True)