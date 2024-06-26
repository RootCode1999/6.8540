package mr

import (
	"fmt"
	"os"
)
import "strconv"

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type JobRequest struct {
	WorkId int
}

func (request JobRequest) String() string {
	return fmt.Sprintf("%v", request.WorkId)
}

type JobResponse struct {
	FilePath string
	JobType  JobType
	NReduce  int
	NMap     int
	Id       int
}

func (response JobResponse) String() string {
	switch response.JobType {
	case MapJob:
		return fmt.Sprintf("{JobType:%v,FilePath:%v,Id:%v,NReduce:%v}", response.JobType, response.FilePath, response.Id, response.NReduce)
	case ReduceJob:
		return fmt.Sprintf("{JobType:%v,Id:%v,NMap:%v,NReduce:%v}", response.JobType, response.Id, response.NMap, response.NReduce)
	case WaitJob, CompleteJob:
		return fmt.Sprintf("{JobType:%v}", response.JobType)
	}
	panic(fmt.Sprintf("unexpected JobType %d", response.JobType))
}

type ReportRequest struct {
	Id    int
	Phase SchedulePhase
}

func (request ReportRequest) String() string {
	return fmt.Sprintf("{Id:%v,SchedulePhase:%v}", request.Id, request.Phase)
}

type ReportResponse struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
