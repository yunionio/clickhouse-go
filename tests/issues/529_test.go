package issues

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"os"
	"path"
	"strconv"
	"testing"
	"time"
)

var ClickHouseContainer testcontainers.Container

func TestMain(m *testing.M) {
	// create a ClickHouse container
	ctx := context.Background()
	cwd, err := os.Getwd()
	if err != nil {
		// can't test without container
		panic(err)
	}

	req := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("clickhouse/clickhouse-server:%s", tests.GetClickHouseTestVersion()),
		ExposedPorts: []string{"9000/tcp"},
		WaitingFor:   wait.ForLog("Ready for connections"),
		Mounts:       testcontainers.Mounts(testcontainers.BindMount(path.Join(cwd, "../custom.xml"), "/etc/clickhouse-server/config.d/custom.xml")),
	}
	ClickHouseContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		// can't test without container
		panic(err)
	}

	p, _ := ClickHouseContainer.MappedPort(ctx, "9000")

	os.Setenv("CLICKHOUSE_DB_PORT", p.Port())
	defer ClickHouseContainer.Terminate(ctx) //nolint
	os.Exit(m.Run())
}

func Test529(t *testing.T) {
	mappedPort, envErr := strconv.Atoi(os.Getenv("CLICKHOUSE_DB_PORT"))
	if envErr != nil {
		t.Fatal("Unable to read port value from environment")
	}
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{fmt.Sprintf("127.0.0.1:%d", mappedPort)},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			//Debug: true,
		})
	)
	if assert.NoError(t, err) {

		const ddl = `
		CREATE TABLE issue_529
		(
			Col1 UInt8, 
			Col2 UInt8
		) Engine Memory
		`
		assert.NoError(t, conn.Exec(ctx, ddl))

		if batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_529"); assert.NoError(t, err) {
			for i := 0; i < 10; i++ {
				if err := batch.Append(uint8(i)+10, uint8(i)+20); !assert.NoError(t, err) {
					return
				}
			}

			// simulate connection interrupt
			stopTimeout := 30 * time.Second
			ClickHouseContainer.Stop(ctx, &stopTimeout)
			if err := batch.Send(); assert.Error(t, err) {
				ClickHouseContainer.Start(ctx)
				// send should be permitted as sent flag is now not set
				assert.NoError(t, batch.Send())
			}
		}
	}
}
