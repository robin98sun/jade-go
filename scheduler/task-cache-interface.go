package scheduler

import (
	"aces/jade-go/kernel"
)

func (c *TaskCache) CacheTaskForSubnode(taskId string, subnode *kernel.Node, moduleName string, taskItem *TaskDispatchingItem, pod *kernel.Pod) {

}

func (c *TaskCache) CheckTask(taskId string) {

}

func (c *TaskCache) SaveResultFromApp(taskId string, subtaskId string, status TaskStatus, result interface{}) {
	// task := c.GetTask(taskId)
	// subtask := task.GetSubtask(subtaskId)

}
