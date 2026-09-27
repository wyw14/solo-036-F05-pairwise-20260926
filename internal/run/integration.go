package run

import "github.com/wyw14/solo-036/internal/model"

// Pressurize 把命令推进到“升压中”；到达目标由升压链再用 Launch 推进到“已发射”。
// 不允许从“已装料”直接跳到“已发射”——必须经过按斜率升压的全过程。
func Pressurize(store *Store, id string) error {
	return store.Transition(id, model.StatePressurizing)
}

// Launch 标记升压完成、清管器已发射。
func Launch(store *Store, id string) error {
	return store.Transition(id, model.StateLaunched)
}

func Track(store *Store, id string) error { return store.Transition(id, model.StateTracking) }
