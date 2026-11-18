package stun

import (
	"net"
	"testing"
	"time"

	"github.com/pion/stun/v3"
)

// A binding request must come back with a success response carrying the
// sender's mapped address.
func TestListenAnswersBindingRequest(t *testing.T) {
	closer, err := Listen("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { closer.Close() })

	serverAddr := closer.(*net.UDPConn).LocalAddr().String()
	client, err := net.Dial("udp", serverAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	req, err := stun.Build(stun.BindingRequest, stun.NewTransactionIDSetter(stun.NewTransactionID()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write(req.Raw); err != nil {
		t.Fatal(err)
	}

	client.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 1500)
	n, err := client.Read(buf)
	if err != nil {
		t.Fatalf("reading the binding response: %v", err)
	}
	resp := &stun.Message{Raw: append([]byte(nil), buf[:n]...)}
	if err := resp.Decode(); err != nil {
		t.Fatalf("decoding the response: %v", err)
	}
	if resp.Type != stun.BindingSuccess {
		t.Fatalf("response type = %v, want %v", resp.Type, stun.BindingSuccess)
	}
	if resp.TransactionID != req.TransactionID {
		t.Errorf("transaction ID = %v, want %v", resp.TransactionID, req.TransactionID)
	}
	var xor stun.XORMappedAddress
	if err := xor.GetFrom(resp); err != nil {
		t.Fatalf("response has no XOR-MAPPED-ADDRESS: %v", err)
	}
	localAddr, ok := client.LocalAddr().(*net.UDPAddr)
	if !ok {
		t.Fatalf("client address is %T, want *net.UDPAddr", client.LocalAddr())
	}
	if xor.Port != localAddr.Port {
		t.Errorf("mapped port = %d, want %d", xor.Port, localAddr.Port)
	}
}

// Non-binding traffic must be ignored rather than answered.
func TestListenIgnoresOtherMessages(t *testing.T) {
	closer, err := Listen("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { closer.Close() })

	client, err := net.Dial("udp", closer.(*net.UDPConn).LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Write([]byte("not a stun message")); err != nil {
		t.Fatal(err)
	}

	client.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if n, err := client.Read(make([]byte, 1500)); err == nil {
		t.Fatalf("server answered junk with %d bytes", n)
	}
}

func TestListenReportsBadAddress(t *testing.T) {
	if _, err := Listen("udp", "not-an-address::"); err == nil {
		t.Fatal("Listen accepted an invalid address")
	}
}
