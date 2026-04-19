import Foundation

// imessage-export: Copies iMessage database to a temp location for syncing.
// This binary should be granted Full Disk Access instead of python3.

let src = NSString("~/Library/Messages/chat.db").expandingTildeInPath
let dst = CommandLine.arguments.count > 1
    ? CommandLine.arguments[1]
    : "/tmp/vadimgest_imessage.db"

let fm = FileManager.default

guard fm.fileExists(atPath: src) else {
    fputs("Error: \(src) not found\n", stderr)
    exit(1)
}

// Remove old copy
try? fm.removeItem(atPath: dst)

do {
    try fm.copyItem(atPath: src, toPath: dst)
    print(dst)
} catch {
    fputs("Error: \(error.localizedDescription)\n", stderr)
    exit(1)
}
