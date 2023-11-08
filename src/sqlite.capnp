@0xadfd26bebe3d83ee;

# Copyright (c) 2023 Vaci Koblizek.
# Licensed under the Apache 2.0 license found in the LICENSE file or at:
#     https://opensource.org/licenses/Apache-2.0

annotation sqliteType @0xab6671fbf244a8de (field): Text;
annotation primaryKey @0xbf80fc3031df0b60 (field): Bool;
annotation columnName @0xa9bcdb16cc5bbc7f (field): Bool;
annotation schema @0x89ea0152d4a3dae3 (struct): Text;
annotation table @0xb337d975d55c655a (struct): Text;
annotation ignore @0xddc3b0b27d076cd1 (field): Bool;

annotation base64 @0xce3cdc2923dc4341 (field) :Void;
# Place on a field of type `Data` to indicate that its representation is a Base64 string.

annotation hex @0x91c5c1f27aad2175 (field) :Void;
# Place on a field of type `Data` to indicate that its representation is a hex string.
