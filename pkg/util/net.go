package util

import (
	"errors"
	"fmt"
	"net"
	"os"
)

func GetLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", errors.New("failed get local ip")
}

func GetIPAndHostname() (string, string, error) {
	ip, err1 := GetLocalIP()
	hostname, err2 := os.Hostname()
	if err1 != nil || err2 != nil {
		return "", "", fmt.Errorf("ip err:%s, hostname err:%s", err1, err2)
	}
	return ip, hostname, nil
}
