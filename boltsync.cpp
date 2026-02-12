#include "boltsync.h"
#include "db.h"

namespace bboltpp {

Error fdatasync(DB* db) {
  if (db == nullptr || db->file == nullptr) {
    return Error{ErrorCode::ErrDatabaseNotOpen};
  }

  return db->file->Sync();
}

}  // namespace bboltpp
