// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2019 Datadog, Inc.

// +build windows

package net

import (
	"errors"
	"fmt"
	"strings"

	"golang.org/x/sys/windows/registry"
)

func getLocalDefinedNTPServers() ([]string, error) {
	regKeyPath := `SYSTEM\CurrentControlSet\Services\W32Time\Parameters`
	k, err := registry.OpenKey(registry.LOCAL_MACHINE, regKeyPath, registry.QUERY_VALUE)
	if err != nil {
		return nil, errors.New("Cannot open registry key: " + regKeyPath)
	}
	defer k.Close()

	regKeyName := "NtpServer"
	s, _, err := k.GetStringValue(regKeyName)
	if err != nil {
		return nil, fmt.Errorf("Cannot get the value %s in registry key: %s (%s)", regKeyName, regKeyPath, err)
	}
	return getNptServersFromRegKeyValue(s), nil
}

func getNptServersFromRegKeyValue(regKeyValue string) []string {
	// Possible formats:
	// time.windows.com,0x9
	// pool.ntp.org time.windows.com time.apple.com time.google.com
	fields := strings.Fields(regKeyValue)
	var servers []string
	for _, f := range fields {
		server := strings.Split(f, ",")[0]
		servers = append(servers, server)
	}

	return servers
}
