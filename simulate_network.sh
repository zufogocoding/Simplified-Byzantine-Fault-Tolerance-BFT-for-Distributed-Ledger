#!/bin/bash
# simulate_network.sh - Mo phong do tre va mat goi tin mang ao cho cac container Docker bang 'tc' (Traffic Control)

INTERFACE="eth0"
DELAY="100ms"
LOSS="5%"

echo "=== MO PHONG DIEU KIEN MANG THUCTE ==="
echo "Giao tiep mang: $INTERFACE"
echo "Do tre: $DELAY"
echo "Ti le mat goi: $LOSS"
echo

if [ "$1" == "on" ]; then
    echo "Kich hoat mo phong tre/mat goi tren interface $INTERFACE..."
    # Them quy tac tc qdisc
    sudo tc qdisc add dev $INTERFACE root netem delay $DELAY loss $LOSS
    echo "Da bat cau hinh mang!"
elif [ "$1" == "off" ]; then
    echo "Tat mo phong mang, khoi phuc mang binh thuong..."
    # Xoa quy tac tc qdisc
    sudo tc qdisc del dev $INTERFACE root
    echo "Da khoi phuc mang binh thuong!"
else
    echo "Su dung:"
    echo "  ./simulate_network.sh on  - Kich hoat delay $DELAY va loss $LOSS"
    echo "  ./simulate_network.sh off - Xoa bo tat ca cau hinh delay/loss"
fi
