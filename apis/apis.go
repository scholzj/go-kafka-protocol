package apis

func RequestHeaderVersion(apiKey int16, apiVersion int16) int16 {
	switch apiKey {
	case 3: // Metadata
		if apiVersion >= 9 {
			return 2
		} else {
			return 1
		}
	case 10: // FindCoordinator
		if apiVersion >= 3 {
			return 2
		} else {
			return 1
		}
	case 18: // ApiVersions
		if apiVersion >= 3 {
			return 2
		} else {
			return 1
		}
	case 60: // DescribeCluster
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	default:
		return 1
	}
}

func ResponseHeaderVersion(apiKey int16, apiVersion int16) int16 {
	switch apiKey {
	case 3: // Metadata
		if apiVersion >= 9 {
			return 1
		} else {
			return 0
		}
	case 10: // FindCoordinator
		if apiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 18: // ApiVersions
		// Always uses response header 0
		return 0
	case 60: // DescribeCluster
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	default:
		return 0
	}
}
