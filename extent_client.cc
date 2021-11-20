// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

extent_client::extent_client(std::string dst) {
    sockaddr_in dstsock;
    make_sockaddr(dst.c_str(), &dstsock);
    cl = new rpcc(dstsock);
    if (cl->bind() != 0) {
        printf("extent_client: bind failed\n");
    }
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id) {
    extent_protocol::status ret = extent_protocol::OK;
    // Your lab2 part1 code goes here
    uint32_t type_t = type;
    extent_protocol::extentid_t id_t = id;

    ret = cl->call(extent_protocol::create, type_t, id_t);

    id = id_t;
    return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf) {
    extent_protocol::status ret = extent_protocol::OK;
    // Your lab2 part1 code goes here
    extent_protocol::extentid_t eid_t = eid;
    ret = cl->call(extent_protocol::get, eid_t, buf);
    return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid,
                       extent_protocol::attr &attr) {
    extent_protocol::status ret = extent_protocol::OK;
    // Your lab2 part1 code goes here
    extent_protocol::extentid_t eid_t = eid;
    ret = cl->call(extent_protocol::getattr, eid_t, attr);
    return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf) {
    extent_protocol::status ret = extent_protocol::OK;
    // Your lab2 part1 code goes here

    int r, empty = 0;
    extent_protocol::extentid_t eid_t = eid;
    ret = cl->call(extent_protocol::put, eid_t, buf, empty);
    return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid) {
    extent_protocol::status ret = extent_protocol::OK;
    // Your lab2 part1 code goes here
    int empty = 0;
    extent_protocol::extentid_t eid_t = eid;
    ret = cl->call(extent_protocol::remove, eid_t, empty);
    return ret;
}
