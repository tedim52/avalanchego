// Copyright (C) 2019-2022, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package uptime

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils"
)

func TestLockedCalculator(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lc := NewLockedCalculator()
	require.NotNil(t)

	// Should still error because ctx is nil
	nodeID := ids.GenerateTestNodeID()
	_, _, err := lc.CalculateUptime(nodeID)
	require.ErrorIs(err, errNotReady)

	_, err = lc.CalculateUptimePercent(nodeID)
	require.ErrorIs(err, errNotReady)

	_, err = lc.CalculateUptimePercentFrom(nodeID, time.Now())
	require.ErrorIs(err, errNotReady)

	var isBootstrapped utils.AtomicBool
	mockCalc := NewMockCalculator(ctrl)

	// Should still error because ctx is not bootstrapped
	lc.SetCalculator(&isBootstrapped, &sync.Mutex{}, mockCalc)
	_, _, err = lc.CalculateUptime(nodeID)
	require.ErrorIs(err, errNotReady)

	_, err = lc.CalculateUptimePercent(nodeID)
	require.ErrorIs(err, errNotReady)

	_, err = lc.CalculateUptimePercentFrom(nodeID, time.Now())
	require.ErrorIs(err, errNotReady)

	isBootstrapped.SetValue(true)

	// Should return the value from the mocked inner calculator
	mockErr := errors.New("mock error")
	mockCalc.EXPECT().CalculateUptime(gomock.Any()).AnyTimes().Return(time.Duration(0), time.Time{}, mockErr)
	_, _, err = lc.CalculateUptime(nodeID)
	require.ErrorIs(err, mockErr)

	mockCalc.EXPECT().CalculateUptimePercent(gomock.Any()).AnyTimes().Return(float64(0), mockErr)
	_, err = lc.CalculateUptimePercent(nodeID)
	require.ErrorIs(err, mockErr)

	mockCalc.EXPECT().CalculateUptimePercentFrom(gomock.Any(), gomock.Any()).AnyTimes().Return(float64(0), mockErr)
	_, err = lc.CalculateUptimePercentFrom(nodeID, time.Now())
	require.ErrorIs(err, mockErr)
}
