package exporter

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/google/cadvisor/info"
	"github.com/google/cadvisor/manager"
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "container"

// Just a small interface for easier housekeeping
type vector interface {
	Reset()
	Describe(ch chan<- *prometheus.Desc)
	Collect(ch chan<- prometheus.Metric)
}

type PrometheusCollector struct {
	mutex   sync.RWMutex
	manager manager.Manager

	errors   prometheus.Counter
	lastSeen *prometheus.CounterVec

	cpuUsageSeconds       *prometheus.CounterVec
	cpuUsageSecondsPerCPU *prometheus.CounterVec

	memoryUsageBytes *prometheus.GaugeVec
	memoryWorkingSet *prometheus.GaugeVec
	memoryFailures   *prometheus.CounterVec

	fsLimit        *prometheus.GaugeVec
	fsUsage        *prometheus.GaugeVec
	fsReads        *prometheus.CounterVec
	fsReadsSectors *prometheus.CounterVec
	fsReadsMerged  *prometheus.CounterVec
	fsReadTime     *prometheus.CounterVec

	fsWrites        *prometheus.CounterVec
	fsWritesSectors *prometheus.CounterVec
	fsWritesMerged  *prometheus.CounterVec
	fsWriteTime     *prometheus.CounterVec

	fsIoInProgress *prometheus.GaugeVec
	fsIoTime       *prometheus.CounterVec

	fsWeightedIoTime *prometheus.CounterVec

	networkRxBytes   *prometheus.CounterVec
	networkRxPackets *prometheus.CounterVec
	networkRxErrors  *prometheus.CounterVec
	networkRxDropped *prometheus.CounterVec
	networkTxBytes   *prometheus.CounterVec
	networkTxPackets *prometheus.CounterVec
	networkTxErrors  *prometheus.CounterVec
	networkTxDropped *prometheus.CounterVec

	tasksSleeping        *prometheus.GaugeVec
	tasksRunning         *prometheus.GaugeVec
	tasksStopped         *prometheus.GaugeVec
	tasksUninterruptible *prometheus.GaugeVec
	tasksIoWait          *prometheus.GaugeVec

	vectors []vector
}

// New returns a new PrometheusCollector
func NewPrometheusCollector(manager manager.Manager) *PrometheusCollector {
	c := &PrometheusCollector{
		manager: manager,
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "errors_total",
			Help:      "Errors while exporting container metrics.",
		}),
		lastSeen: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "last_seen",
			Help:      "Last time a container was seen by the exporter",
		},
			[]string{"name", "id", "image"},
		),
		cpuUsageSeconds: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "cpu_usage_seconds_total",
			Help:      "Total seconds of cpu time consumed.",
		},
			[]string{"name", "id", "image", "type"},
		),
		cpuUsageSecondsPerCPU: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "cpu_usage_per_cpu_seconds_total",
			Help:      "Total seconds of cpu time consumed per cpu.",
		},
			[]string{"name", "id", "image", "cpu"},
		),

		memoryUsageBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "memory_usage_bytes",
			Help:      "Current memory usage in bytes.",
		},
			[]string{"name", "id", "image"},
		),
		memoryWorkingSet: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "memory_working_set_bytes",
			Help:      "Current working set in bytes.",
		},
			[]string{"name", "id", "image"},
		),
		memoryFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "memory_failures_total",
			Help:      "Number of times memory usage hits limits.",
		},
			[]string{"name", "id", "image", "type", "scope"},
		),

		fsLimit: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "fs_limit_bytes",
			Help:      "Number of bytes that can be consumed by the container on this filesystem.",
		},
			[]string{"name", "id", "image", "device"},
		),
		fsUsage: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "fs_usage_bytes",
			Help:      "Number of bytes that is consumed by the container on this filesystem.",
		},
			[]string{"name", "id", "image", "device"},
		),
		fsReads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "fs_reads_total",
			Help:      "Number of reads completed",
		},
			[]string{"name", "id", "image", "device"},
		),
		fsReadsSectors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "fs_sector_reads_total",
			Help:      "Number of sectors reads completed",
		},
			[]string{"name", "id", "image", "device"},
		),
		fsReadsMerged: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "fs_reads_merged_total",
			Help:      "Number of reads merged",
		},
			[]string{"name", "id", "image", "device"},
		),
		fsReadTime: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "fs_read_seconds",
			Help:      "Number of seconds spent reading",
		},
			[]string{"name", "id", "image", "device"},
		),
		fsWrites: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "fs_writes_total",
			Help:      "Number of writes completed",
		},
			[]string{"name", "id", "image", "device"},
		),
		fsWritesSectors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "fs_sector_writes_total",
			Help:      "Number of sectors writes completed",
		},
			[]string{"name", "id", "image", "device"},
		),
		fsWritesMerged: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "fs_writes_merged_total",
			Help:      "Number of writes merged",
		},
			[]string{"name", "id", "image", "device"},
		),
		fsWriteTime: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "fs_write_seconds",
			Help:      "Number of seconds spent writing",
		},
			[]string{"name", "id", "image", "device"},
		),
		fsIoInProgress: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "fs_io_current",
			Help:      "Number of I/Os currently in progress",
		},
			[]string{"name", "id", "image", "device"},
		),
		fsIoTime: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "fs_io_time_seconds_total",
			Help:      "Number of seconds spent doing I/Os",
		},
			[]string{"name", "id", "image", "device"},
		),
		fsWeightedIoTime: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "fs_io_time_weighted_seconds_total",
			Help:      "Number of seconds spent doing I/Os",
		},
			[]string{"name", "id", "image", "device"},
		),
		networkRxBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "network_receive_bytes_total",
			Help:      "Cumulative count of bytes received",
		},
			[]string{"name", "id", "image"},
		),
		networkRxPackets: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "network_receive_packets_total",
			Help:      "Cumulative count of packets received",
		},
			[]string{"name", "id", "image"},
		),
		networkRxDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "network_receive_packets_dropped_total",
			Help:      "Cumulative count of bytes received",
		},
			[]string{"name", "id", "image"},
		),
		networkRxErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "network_receive_errors_total",
			Help:      "Cumulative count of errors encountered",
		},
			[]string{"name", "id", "image"},
		),
		networkTxBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "network_transmit_bytes_total",
			Help:      "Cumulative count of bytes transmitd",
		},
			[]string{"name", "id", "image"},
		),
		networkTxPackets: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "network_transmit_packets_total",
			Help:      "Cumulative count of packets transmitd",
		},
			[]string{"name", "id", "image"},
		),
		networkTxDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "network_transmit_packets_dropped_total",
			Help:      "Cumulative count of bytes transmitd",
		},
			[]string{"name", "id", "image"},
		),
		networkTxErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "network_transmit_errors_total",
			Help:      "Cumulative count of errors encountered",
		},
			[]string{"name", "id", "image"},
		),

		tasksSleeping: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "tasks_sleeping",
			Help:      "Number of sleeping tasks",
		},
			[]string{"name", "id", "image"},
		),
		tasksRunning: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "tasks_running",
			Help:      "Number of running tasks",
		},
			[]string{"name", "id", "image"},
		),
		tasksStopped: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "tasks_stopped",
			Help:      "Number of stopped tasks",
		},
			[]string{"name", "id", "image"},
		),
		tasksUninterruptible: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "tasks_uinterruptible",
			Help:      "Number of tasks in uninterruptible state",
		},
			[]string{"name", "id", "image"},
		),

		tasksIoWait: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "tasks_io_wait",
			Help:      "Number of tasks waiting on IO",
		},
			[]string{"name", "id", "image"},
		),
	}
	c.vectors = []vector{
		c.lastSeen,

		c.cpuUsageSeconds,
		c.cpuUsageSecondsPerCPU,

		c.memoryUsageBytes,
		c.memoryWorkingSet,
		c.memoryFailures,

		c.fsLimit,
		c.fsUsage,
		c.fsReads,
		c.fsReadsSectors,
		c.fsReadsMerged,
		c.fsReadTime,
		c.fsWrites,
		c.fsWritesSectors,
		c.fsWritesMerged,
		c.fsWriteTime,
		c.fsIoInProgress,
		c.fsIoTime,
		c.fsWeightedIoTime,

		c.networkRxBytes,
		c.networkRxPackets,
		c.networkRxErrors,
		c.networkRxDropped,
		c.networkTxBytes,
		c.networkTxPackets,
		c.networkTxErrors,
		c.networkTxDropped,

		c.tasksSleeping,
		c.tasksRunning,
		c.tasksStopped,
		c.tasksUninterruptible,
		c.tasksIoWait,
	}
	return c
}

// Describe describes all the metrics ever exported by cadvisor. It
// implements prometheus.PrometheusCollector.
func (c *PrometheusCollector) Describe(ch chan<- *prometheus.Desc) {
	c.errors.Describe(ch)
	for _, v := range c.vectors {
		v.Describe(ch)
	}
}

// Collect fetches the stats from all containers and delivers them as
// Prometheus metrics. It implements prometheus.PrometheusCollector.
func (c *PrometheusCollector) Collect(ch chan<- prometheus.Metric) {
	c.mutex.Lock() // To protect metrics from concurrent collects.
	defer c.mutex.Unlock()

	for _, v := range c.vectors {
		v.Reset()
	}

	containers, err := c.manager.SubcontainersInfo("/", &info.ContainerInfoRequest{NumStats: 1})
	if err != nil {
		c.errors.Inc()
		glog.Warning("Couldn't get containers: %s", err)
		return
	}
	for _, container := range containers {
		id := container.Name
		name := strings.Join(container.Aliases, ",")
		stats := container.Stats[0]
		image := "undefined" // FIXME:

		for i, value := range stats.Cpu.Usage.PerCpu {
			c.cpuUsageSecondsPerCPU.WithLabelValues(name, id, image, fmt.Sprintf("cpu%02d", i)).Set(float64(value) / float64(time.Second))
		}

		c.cpuUsageSeconds.WithLabelValues(name, id, image, "kernel").Set(float64(stats.Cpu.Usage.System) / float64(time.Second))
		c.cpuUsageSeconds.WithLabelValues(name, id, image, "user").Set(float64(stats.Cpu.Usage.User) / float64(time.Second))

		// FIXME: What about load average?
		c.memoryFailures.WithLabelValues(name, id, image, "pgfault", "container").Set(float64(stats.Memory.ContainerData.Pgfault))
		c.memoryFailures.WithLabelValues(name, id, image, "pgmajfault", "container").Set(float64(stats.Memory.ContainerData.Pgmajfault))
		c.memoryFailures.WithLabelValues(name, id, image, "pgfault", "hierarchy").Set(float64(stats.Memory.HierarchicalData.Pgfault))
		c.memoryFailures.WithLabelValues(name, id, image, "pgmajfault", "hierarchy").Set(float64(stats.Memory.HierarchicalData.Pgmajfault))

		for metric, value := range map[*prometheus.GaugeVec]uint64{
			c.memoryUsageBytes:     stats.Memory.Usage,
			c.memoryWorkingSet:     stats.Memory.WorkingSet,
			c.tasksSleeping:        stats.TaskStats.NrSleeping,
			c.tasksRunning:         stats.TaskStats.NrRunning,
			c.tasksStopped:         stats.TaskStats.NrStopped,
			c.tasksUninterruptible: stats.TaskStats.NrUinterruptible,
			c.tasksIoWait:          stats.TaskStats.NrIoWait,
		} {
			metric.WithLabelValues(name, id, image).Set(float64(value))
		}

		for metric, value := range map[*prometheus.CounterVec]uint64{
			c.lastSeen: uint64(time.Now().Unix()),

			c.networkRxBytes:   stats.Network.RxBytes,
			c.networkRxPackets: stats.Network.RxPackets,
			c.networkRxErrors:  stats.Network.RxErrors,
			c.networkRxDropped: stats.Network.RxDropped,
			c.networkTxBytes:   stats.Network.TxBytes,
			c.networkTxPackets: stats.Network.TxPackets,
			c.networkTxErrors:  stats.Network.TxErrors,
			c.networkTxDropped: stats.Network.TxDropped,
		} {
			metric.WithLabelValues(name, id, image).Set(float64(value))
		}

		for _, stat := range stats.Filesystem {
			for metric, value := range map[*prometheus.CounterVec]float64{
				c.fsReads:        float64(stat.ReadsCompleted),
				c.fsReadsSectors: float64(stat.SectorsRead),
				c.fsReadsMerged:  float64(stat.ReadsMerged),
				c.fsReadTime:     float64(stat.ReadTime) / float64(time.Second),

				c.fsWrites:        float64(stat.WritesCompleted),
				c.fsWritesSectors: float64(stat.SectorsWritten),
				c.fsWritesMerged:  float64(stat.WritesMerged),
				c.fsWriteTime:     float64(stat.WriteTime) / float64(time.Second),

				c.fsIoTime:         float64(stat.IoInProgress) / float64(time.Second),
				c.fsWeightedIoTime: float64(stat.IoTime) / float64(time.Second),
			} {
				metric.WithLabelValues(name, id, image, stat.Device).Set(value)
			}
			for metric, value := range map[*prometheus.GaugeVec]uint64{
				c.fsIoInProgress: stat.IoInProgress,
				c.fsLimit:        stat.Limit,
				c.fsUsage:        stat.Usage,
			} {
				metric.WithLabelValues(name, id, image, stat.Device).Set(float64(value))
			}
		}
	}

	c.errors.Collect(ch)

	for _, v := range c.vectors {
		v.Collect(ch)
	}
}
