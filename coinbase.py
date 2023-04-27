import time
import requests
import socket
import sys

TCP_IP = '127.0.0.1'
TCP_PORT = 9000

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()

while True:
    url = 'https://api.pro.coinbase.com/products/BTC-USD/ticker'
    response = requests.get(url)
    data = response.json()
    latest_price = data['price']
    conn.send((str(latest_price)+"\n").encode())

conn.close()
