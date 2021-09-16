package shardctrler

// out of time
func (sc *ShardCtrler) BalanceLeave(config *Config, leaveGids []int) {
	length := len(config.Groups)
	if length == 0 {
		config.Shards = [10]int{}
		return
	}
	average := NShards / length
	subNum := NShards - average*length
	avgNum := length - subNum
	// len(config.Groups) - subNum --> average
	// subNum --> average+1
	//DPrintf("length %d, average %d", length, average)

	// countArray should not be have Gid 0
	// TODO 0 --> zeroAimGid
	zeroAimGid := 0
	for gid, _ := range config.Groups {
		if gid != 0 {
			zeroAimGid = gid
		}
	}

	for shards, gid := range config.Shards {
		gidLeave := false
		for _,lgid := range leaveGids {
			if lgid == gid {
				gidLeave = true
				break
			}
		}
		if gid == 0 || gidLeave {
			config.Shards[shards] = zeroAimGid
		}
	}

	// count Every Gid mangement which Shard ???
	countArray := make(map[int][]int)
	for shardIndex, gid := range config.Shards {
		if _, exist := countArray[gid]; exist {
			countArray[gid] = append(countArray[gid], shardIndex)
		} else {
			countArray[gid] = make([]int, 0)
			countArray[gid] = append(countArray[gid], shardIndex)
		}
	}
	for gid, _ := range config.Groups {
		if _, exist := countArray[gid]; !exist {
			countArray[gid] = make([]int, 0)
		}
	}

	//DPrintf("====countArray=====")
	//for gid, shards := range countArray {
	//	DPrintf("[countarrya]gid %d, shards %v", gid, shards)
	//}

	for {
		if ifBalance(average, avgNum, subNum, countArray) {
			break
		}
		// make Max Gid One Shard to Min Gid
		maxShardsNum := -1
		maxGid := -1
		minShardsNum := NShards * 10
		minGid := -1
		for gid, shardsArray := range countArray {
			if len(shardsArray) >= maxShardsNum {
				maxShardsNum = len(shardsArray)
				maxGid = gid
			}
			if len(shardsArray) <= minShardsNum {
				minShardsNum = len(shardsArray)
				minGid = gid
			}
		}

		fromGid := maxGid
		movedShard := countArray[maxGid][maxShardsNum-1]
		toGid := minGid

		//DPrintf("[Blance]Shard %d from Gid %d ===> Gid %d",movedShard, fromGid,toGid)
		countArray[fromGid] = countArray[fromGid][:maxShardsNum-1]
		countArray[toGid] = append(countArray[toGid], movedShard)
		config.Shards[movedShard] = toGid
	}
}

// out of time
func (sc *ShardCtrler) BalanceJoin(config *Config, newSevers map[int][]string) {
	length := len(config.Groups)
	_,ifLeader := sc.rf.GetState()
	if length >= 10 && ifLeader{
		//DPrintf("[Join After]Groups %d", length)
		//for gid,servers := range config.Groups{
		//	DPrintf("[MoreThen10]gid %d, servers %v",gid,servers)
		//}
	}
	average := NShards / length
	subNum := NShards - average*length
	avgNum := length - subNum
	// len(config.Groups) - subNum --> average
	// subNum --> average+1
	//DPrintf("length %d, average %d", length, average)

	// countArray should not be have Gid 0
	// TODO 0 --> zeroAimGid
	zeroAimGid := 0
	for gid, _ := range config.Groups {
		if gid != 0 {
			zeroAimGid = gid
		}
	}
	for shards, gid := range config.Shards {
		if gid == 0 {
			config.Shards[shards] = zeroAimGid
		}
	}

	// count Every Gid mangement which Shard ???
	countArray := make(map[int][]int)
	for shardIndex, gid := range config.Shards {
		if _, exist := countArray[gid]; exist {
			countArray[gid] = append(countArray[gid], shardIndex)
		} else {
			countArray[gid] = make([]int, 0)
			countArray[gid] = append(countArray[gid], shardIndex)
		}
	}
	if len(countArray) >= 10 {
		return
	}

	for gid, _ := range newSevers {
		if _, exist := countArray[gid]; !exist {
			countArray[gid] = make([]int, 0)
		}
	}

	//DPrintf("====countArray=====")
	//for gid, shards := range countArray {
	//	DPrintf("[countarrya]gid %d, shards %v", gid, shards)
	//}

	for {
		if ifBalance(average, avgNum, subNum, countArray) {
			break
		}
		// make Max Gid One Shard to Min Gid
		maxShardsNum := -1
		maxGid := -1
		minShardsNum := NShards * 10
		minGid := -1
		for gid, shardsArray := range countArray {
			if len(shardsArray) >= maxShardsNum {
				maxShardsNum = len(shardsArray)
				maxGid = gid
			}
			if len(shardsArray) <= minShardsNum {
				minShardsNum = len(shardsArray)
				minGid = gid
			}
		}

		fromGid := maxGid
		movedShard := countArray[maxGid][maxShardsNum-1]
		toGid := minGid

		//DPrintf("[Blance]Shard %d from Gid %d ===> Gid %d",movedShard, fromGid,toGid)
		countArray[fromGid] = countArray[fromGid][:maxShardsNum-1]
		countArray[toGid] = append(countArray[toGid], movedShard)
		config.Shards[movedShard] = toGid
	}
}


func realNumArray(GidToShardNumMap map[int]int) []int {
	length := len(GidToShardNumMap)

	numArray := make([]int, 0, length)
	for gid, _ := range GidToShardNumMap {
		numArray = append(numArray, gid)
	}

	for i := 0; i < length-1; i++ {
		for j := length - 1; j > i; j-- {
			if GidToShardNumMap[numArray[j]] < GidToShardNumMap[numArray[j-1]] || (GidToShardNumMap[numArray[j]] == GidToShardNumMap[numArray[j-1]] && numArray[j] < numArray[j-1]) {
				numArray[j], numArray[j-1] = numArray[j-1],numArray[j]
			}
		}
	}
	return numArray
}

func ifAvg(length int, subNum int, i int) bool{
	if i < length-subNum{
		return true
	}else {
		return false
	}
}

func (sc *ShardCtrler) reBalanceShards(GidToShardNumMap map[int]int, lastShards [NShards]int) [NShards]int {
	length := len(GidToShardNumMap)
	average := NShards / length
	subNum := NShards % length
	realSortNum := realNumArray(GidToShardNumMap)

	for i := length - 1; i >= 0; i-- {
		resultNum := average
		if !ifAvg(length,subNum,i){
			resultNum = average+1
		}
		if resultNum < GidToShardNumMap[realSortNum[i]] {
			fromGid := realSortNum[i]
			changeNum := GidToShardNumMap[fromGid] - resultNum
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == fromGid {
					lastShards[shard] = 0
					changeNum -= 1
				}
			}
			GidToShardNumMap[fromGid] = resultNum
		}
	}

	for i := 0; i < length; i++ {
		resultNum := average
		if !ifAvg(length,subNum,i){
			resultNum = average+1
		}
		if resultNum > GidToShardNumMap[realSortNum[i]] {
			toGid := realSortNum[i]
			changeNum := resultNum - GidToShardNumMap[toGid]
			for shard, gid := range lastShards{
				if changeNum <= 0 {
					break
				}
				if gid == 0 {
					lastShards[shard] = toGid
					changeNum -= 1
				}
			}
			GidToShardNumMap[toGid] = resultNum
		}

	}
	return lastShards
}

func (sc *ShardCtrler) MakeMoveConfig(shard int, gid int) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	tempConfig := Config{Num: len(sc.configs),
						Shards: [10]int{},
						Groups: map[int][]string{}}
	for shards, gids := range lastConfig.Shards {
		tempConfig.Shards[shards] = gids
	}
	tempConfig.Shards[shard] = gid

	for gidss, servers := range lastConfig.Groups {
		tempConfig.Groups[gidss] = servers
	}

	return &tempConfig
}

func (sc *ShardCtrler) MakeJoinConfig(servers map[int][]string) *Config {

	lastConfig := sc.configs[len(sc.configs)-1]
	tempGroups := make(map[int][]string)
	
	for gid, serverList := range lastConfig.Groups {
		tempGroups[gid] = serverList
	}
	for gids, serverLists := range servers {
		tempGroups[gids] = serverLists
	}

	GidToShardNumMap := make(map[int]int)
	for gid := range tempGroups {
		GidToShardNumMap[gid] = 0
	}
	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			GidToShardNumMap[gid]++
		}

	}

	if len(GidToShardNumMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: tempGroups,
		}
	}
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.reBalanceShards(GidToShardNumMap,lastConfig.Shards),
		Groups: tempGroups,
	}
}

func (sc *ShardCtrler) MakeLeaveConfig(gids []int) *Config {

	lastConfig := sc.configs[len(sc.configs)-1]
	tempGroups := make(map[int][]string)

	ifLeaveSet := make(map[int]bool)
	for _, gid := range gids {
		ifLeaveSet[gid] = true
	}

	for gid, serverList := range lastConfig.Groups {
		tempGroups[gid] = serverList
	}
	for _, gidLeave := range gids {
		delete(tempGroups,gidLeave)
	}

	newShard := lastConfig.Shards
	GidToShardNumMap := make(map[int]int)
	for gid := range tempGroups {
		if !ifLeaveSet[gid] {
			GidToShardNumMap[gid] = 0
		}

	}
	for shard, gid := range lastConfig.Shards {
		if gid != 0 {
			if ifLeaveSet[gid] {
				newShard[shard] = 0
			} else {
				GidToShardNumMap[gid]++
			}
		}

	}
	if len(GidToShardNumMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: tempGroups,
		}
	}
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.reBalanceShards(GidToShardNumMap,newShard),
		Groups: tempGroups,
	}
}



