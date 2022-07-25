package net;


import common.RequestOrResponse;

//<codeFragment name="clientConnection">
public interface ClientConnection {
    void write(RequestOrResponse response);
    void close();
}
//</codeFragment>