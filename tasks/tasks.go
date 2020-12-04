package tasks

import (
	"context"
	"datawaves/util"
	"fmt"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
)

var (
	appID  string
	client *cloudtasks.Client
)

func init() {
	if util.IsProduction() {
		appID = "datawaves"
	} else {
		appID = "datawaves-testing"
	}
}

// https://cloud.google.com/tasks/docs/creating-http-target-tasks
// This tasks saves reads and writes to elasticsearch
func AddUsageTask(idx, message string) (*taskspb.Task, error) {
	ctx := context.Background()

	client, err := cloudtasks.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("NewClient: %v", err)
	}
	defer client.Close()

	queuePath := fmt.Sprintf("projects/%s/locations/%s/queues/%s", appID, "us-central1", "usage")
	req := &taskspb.CreateTaskRequest{
		Parent: queuePath,
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_AppEngineHttpRequest{
				AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
					HttpMethod:  taskspb.HttpMethod_POST,
					RelativeUri: "/tasks/record_usage",
				},
			},
		},
	}

	// Add a payload message if one is present.
	req.Task.GetAppEngineHttpRequest().Body = []byte(message)

	createdTask, err := client.CreateTask(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("cloudtasks.CreateTask: %v", err)
	}

	return createdTask, nil
}

// This tasks writes to firestore database
func AddWriteTask(projectID string) (*taskspb.Task, error) {
	ctx := context.Background()

	client, err := cloudtasks.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("NewClient: %v", err)
	}
	defer client.Close()

	queuePath := fmt.Sprintf("projects/%s/locations/%s/queues/%s", appID, "us-central1", "writes")
	req := &taskspb.CreateTaskRequest{
		Parent: queuePath,
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_AppEngineHttpRequest{
				AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
					HttpMethod:  taskspb.HttpMethod_POST,
					RelativeUri: "/tasks/record_writes",
				},
			},
		},
	}

	// Add a payload message if one is present.
	req.Task.GetAppEngineHttpRequest().Body = []byte(fmt.Sprintf("projectID=%s", projectID))

	createdTask, err := client.CreateTask(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("cloudtasks.CreateTask: %v", err)
	}

	return createdTask, nil
}
