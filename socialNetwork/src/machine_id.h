#ifndef SOCIAL_NETWORK_MICROSERVICES_MACHINE_ID_H
#define SOCIAL_NETWORK_MICROSERVICES_MACHINE_ID_H

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netinet/in.h>

#include <cstdio>
#include <string>

#include "logger.h"

namespace social_network {

/*
 * The 3-hex-digit machine id that prefixes the ids a service generates, from the
 * low 11 bits of the IPv4 address of <netif> (the ids drop the top bit of the
 * 64-bit value, which is the machine id's top bit). Instances whose addresses lie
 * in one /21 (for example the pods of a Kubernetes node's /24 pod subnet, or a
 * Docker network up to /21) get distinct ids, so replicas never generate the
 * same id.
 * Returns "" when <netif> has no IPv4 address.
 */
std::string MachineIdFromIPv4(const std::string &netif) {
  struct ifaddrs *addresses = nullptr;
  if (getifaddrs(&addresses) != 0) {
    return "";
  }
  std::string machine_id;
  for (struct ifaddrs *entry = addresses; entry != nullptr; entry = entry->ifa_next) {
    if (entry->ifa_addr == nullptr || entry->ifa_addr->sa_family != AF_INET ||
        netif != entry->ifa_name) {
      continue;
    }
    uint32_t address = ntohl(
        reinterpret_cast<struct sockaddr_in *>(entry->ifa_addr)->sin_addr.s_addr);
    char buffer[8];
    snprintf(buffer, sizeof(buffer), "%03x", address & 0x7ff);
    machine_id = buffer;
    LOG(info) << "machine id " << machine_id << " from the IPv4 address of " << netif;
    break;
  }
  freeifaddrs(addresses);
  return machine_id;
}

}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_MACHINE_ID_H
