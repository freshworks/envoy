import Foundation

/// The different ways Envoy Mobile can monitor network reachability
/// state.
@objc
public enum NetworkMonitoringMode: Int {
  /// Do not monitor changes to the network reachability state.
  case disabled = 0
  /// Monitor changes to the network reachability state using `SCNetworkReachability`.
  case reachability = 1
  /// Monitor changes to the network reachability state using `NWPathMonitor`.
  case pathMonitor = 2
  /// Monitor changes to the network reachability state using `NWPathMonitor`.
  /// Use experimental improvements: avoid forcing the DNS update on the initial
  /// reachability update.
  case pathMonitorExperimental = 3
}
