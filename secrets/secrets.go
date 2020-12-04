package secrets

import (
	secretmanager "cloud.google.com/go/secretmanager/apiv1beta1"
	"context"
	"datawaves/errors"
	"datawaves/util"
	"fmt"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1beta1"
	"os"
)

func Set(id, secret string) error {

	var projectID string

	if util.IsProduction() {
		projectID = os.Getenv("production_id")
	} else {
		projectID = os.Getenv("testing_id")
	}

	ctx := context.Background()
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		errors.Log(err)
		return errors.New(fmt.Sprintf("failed to create secretmanager client: %v", err))
	}

	parent := "projects/" + projectID

	req := &secretmanagerpb.CreateSecretRequest{
		Parent:   parent,
		SecretId: id,
		Secret: &secretmanagerpb.Secret{
			Replication: &secretmanagerpb.Replication{
				Replication: &secretmanagerpb.Replication_Automatic_{
					Automatic: &secretmanagerpb.Replication_Automatic{},
				},
			},
		},
	}

	_, err = client.CreateSecret(ctx, req)
	if err != nil {
		errors.Log(err)
		return errors.New(fmt.Sprintf("failed to create secret: %v", err))
	}

	payload := []byte(secret)

	client, err = secretmanager.NewClient(ctx)
	if err != nil {
		errors.Log(err)
		return errors.New(fmt.Sprintf("failed to create secretmanager client: %v", err))
	}

	parent = "projects/" + projectID + "/secrets/" + id

	rq := &secretmanagerpb.AddSecretVersionRequest{
		Parent: parent,
		Payload: &secretmanagerpb.SecretPayload{
			Data: payload,
		},
	}

	_, err = client.AddSecretVersion(ctx, rq)
	if err != nil {
		errors.Log(err)
		return errors.New(fmt.Sprintf("failed to add secret version: %v", err))
	}

	return nil
}

func Get(id string) (string, error) {
	ctx := context.Background()

	var projectID string

	if util.IsProduction() {
		projectID = os.Getenv("production_id")
	} else {
		projectID = os.Getenv("testing_id")
	}

	name := "projects/" + projectID + "/secrets/" + id + "/versions/latest"

	req := &secretmanagerpb.AccessSecretVersionRequest{
		Name: name,
	}

	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		errors.Log(err)
		return "", errors.New(fmt.Sprintf("failed to create secretmanager client: %v", err))
	}

	result, err := client.AccessSecretVersion(ctx, req)
	if err != nil {
		errors.Log(err)
		return "", errors.New(fmt.Sprintf("failed to access secret version: %v", err))
	}

	return string(result.Payload.Data), nil
}
